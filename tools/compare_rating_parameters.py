"""修正済みratingの係数を、日付分割で比較する読み取り専用ツール。"""
from __future__ import annotations

import argparse
import contextlib
from dataclasses import asdict
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT / 'etc_py/00_Export_To_Excel_4.py'
TRAIN_END = '20251231'
VALID_END = '20260529'


def split_dates(dates):
    # 日付で分割し、同日のレースを別々の区間へ振り分けない。
    return np.where(dates <= TRAIN_END, 'train', np.where(dates <= VALID_END, 'validation', 'test'))


def load_exporter():
    # CLIのExcel書き出しを起動せず、評価計算だけを呼ぶ。
    spec = importlib.util.spec_from_file_location('rating_parameter_comparison', SOURCE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def joined_frames(store):
    entries = pd.DataFrame(store.entries)
    history = pd.DataFrame(store.ratings_history)
    levels = pd.DataFrame(store.race_levels)
    # 取消は履歴に含まれないため、出走配列の位置ではなく一意キーで結合する。
    cols = ['race_id','horse_id','rank','pop','odds']
    history = history.merge(entries[cols],on=['race_id','horse_id'],validate='one_to_one')
    history = history.merge(levels[['race_id','field_size','class']],on='race_id',validate='many_to_one')
    assert levels.race_id.is_unique and entries[['race_id','horse_id']].duplicated().sum()==0
    return history, levels, entries


def per_race_metrics(history, feature='pre_rating', ascending=False):
    frame = history.loc[history['rank'].gt(0)].copy()
    groups = frame.groupby('race_id')
    rating_rank = groups[feature].rank(method='average',ascending=ascending)
    actual_rank = groups['rank'].rank(method='average')
    rating_centered = rating_rank-rating_rank.groupby(frame.race_id).transform('mean')
    actual_centered = actual_rank-actual_rank.groupby(frame.race_id).transform('mean')
    covariance = (rating_centered*actual_centered).groupby(frame.race_id).sum()
    variance = ((rating_centered**2).groupby(frame.race_id).sum()*(actual_centered**2).groupby(frame.race_id).sum())**0.5
    correlations = covariance / variance.replace(0,np.nan)
    best = groups[feature].transform('min' if ascending else 'max')
    selected = frame.loc[frame[feature]==best].copy()
    selected['win'] = (selected['rank']==1).astype(float)
    selected['place'] = (selected['rank']<=3).astype(float)
    scores = selected.groupby('race_id').agg(win=('win','mean'),place=('place','mean'),date=('date','first'))
    scores['spearman'] = correlations
    scores['split'] = split_dates(scores.date)
    return scores


def summarize_scores(scores):
    # 主指標はレース内順位相関。勝率と3着内率は補助指標として併記する。
    return [{'split':split,'races':len(g),'mean_spearman':float(g.spearman.mean()),'top1_win_rate':float(g.win.mean()),'top1_top3_rate':float(g.place.mean())} for split,g in scores.groupby('split')]


def bootstrap_gain(candidate, baseline, column):
    # 同日の相関を保つため、レース単位ではなく開催日単位で再抽出する。
    joined = candidate[[column,'date']].join(baseline[[column]],rsuffix='_base').dropna()
    joined['gain'] = joined[column]-joined[column+'_base']
    daily = joined.groupby('date').agg(total=('gain','sum'),count=('gain','size'))
    rng = np.random.default_rng(20261009)
    indices = rng.integers(0,len(daily),size=(1000,len(daily)))
    gains = daily.total.to_numpy()[indices].sum(axis=1)/daily['count'].to_numpy()[indices].sum(axis=1)
    return {'mean_gain':float(joined.gain.mean()),'ci95_low':float(np.quantile(gains,0.025)),'ci95_high':float(np.quantile(gains,0.975)),'dates':len(daily)}


def compare_levels(history, levels):
    # 目的変数は「実際の次走」の成績。中止を飛ばして次々走へ結び付けない。
    h = history.sort_values(['date','race_id'],kind='stable').copy()
    h['finish_score'] = 1-(h['rank']-1)/(h.field_size-1)
    h['next_finish'] = h.groupby('horse_id').finish_score.shift(-1)
    h['next_date'] = h.groupby('horse_id').date.shift(-1)
    h = h.loc[h['rank'].gt(0)&h.next_finish.notna()&(h.next_date>h.date)].copy()
    h['log_pop'] = np.log1p(pd.to_numeric(h['pop'],errors='coerce'))
    h['own_pre'] = (h.pre_rating-1500)/100
    base = pd.concat([h[['finish_score','log_pop','own_pre']].fillna(0),pd.get_dummies(h['class'].fillna('unknown'),dtype=float)],axis=1)
    train = h.next_date <= TRAIN_END
    validation = (h.date > TRAIN_END)&(h.next_date<=VALID_END)
    test = h.date>VALID_END
    y = h.next_finish.to_numpy()
    by_id = levels.set_index('race_id')
    weights = {
        'default':(0.35,0.25,0.25,0.15),
        'top5_only':(0,1,0,0),
        'mean_only':(0,0,1,0),
        'median_only':(0,0,0,1),
        'balanced':(0.25,0.25,0.25,0.25),
        'top_heavy':(0.6,0.3,0.1,0),
    }
    records = []
    predictions = {}
    candidates = [('no_level',None,0.0)]+[(name,w,scale) for name,w in weights.items() for scale in [0,0.5,1,1.5]]
    for name,w,scale in candidates:
        features = base.copy()
        if w is not None:
            value = sum(by_id[col]*weight for col,weight in zip(['pre_top3_mean','pre_top5_mean','pre_mean','pre_p50'],w))
            value = value+by_id.result_time_level_adjustment.fillna(0)*scale
            features['race_level'] = (h.race_id.map(value)-1500)/100
        x = np.column_stack([np.ones(len(h)),features.to_numpy(dtype=float)])
        penalty = np.eye(x.shape[1])*1e-3
        penalty[0,0] = 0
        # 係数は学習期間だけで算出し、検証・テストの正解は参照しない。
        beta = np.linalg.solve(x[train].T@x[train]+penalty,x[train].T@y[train])
        prediction = x@beta
        key = f'{name}_clock{scale:g}'
        predictions[key] = prediction
        for split,mask in [('train',train),('validation',validation),('test',test)]:
            mse = float(np.mean((prediction[mask]-y[mask])**2))
            a,b = pd.Series(prediction[mask]).rank(),pd.Series(y[mask]).rank()
            records.append({'candidate':key,'weight_name':name,'weights':w,'clock_scale':scale,'split':split,'starts':int(mask.sum()),'mse':mse,'spearman':float(a.corr(b))})
    frame = pd.DataFrame(records)
    # 候補選択は検証期間の誤差だけで固定し、その後の結果で選び直さない。
    best = frame.loc[frame.split=='validation'].sort_values(['mse','candidate']).iloc[0]
    key = best.candidate
    baseline_key = 'default_clock1'
    gain = (predictions[baseline_key][test]-y[test])**2-(predictions[key][test]-y[test])**2
    daily = pd.DataFrame({'date':h.loc[test,'date'].to_numpy(),'gain':gain}).groupby('date').agg(total=('gain','sum'),count=('gain','size'))
    rng = np.random.default_rng(20261009)
    indices = rng.integers(0,len(daily),size=(1000,len(daily)))
    gains = daily.total.to_numpy()[indices].sum(axis=1)/daily['count'].to_numpy()[indices].sum(axis=1)
    result = {'selected_by_validation':key,'weights':best.weights,'clock_scale':float(best.clock_scale),'test_mse_improvement_vs_default':float(gain.mean()),'test_gain_ci95':[float(np.quantile(gains,0.025)),float(np.quantile(gains,0.975))]}
    return frame,result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True,exist_ok=True)
    module = load_exporter()
    src = PROJECT/'data/master/racedata_results.xlsx'
    inputs = [SOURCE,src,module.PLACE_BABA_TIME_PATH,module.GRADE_RACE_MASTER_PATH]
    hashes = {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    (out/'input_hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf-8')
    reader = pd.read_excel
    cache = {}
    def cached_read(*a,**kw):
        # 各候補で同じ元データを使い、読み込み時間だけを節約する。
        key = (str(getattr(a[0],'io',a[0])),str(kw.get('sheet_name',0)))
        if key not in cache:
            cache[key] = reader(*a,**kw)
        return cache[key].copy()
    pd.read_excel = cached_read
    variants = [('default',module.RatingParameters())]
    variants += [(f'surface{surface:g}_clock{clock:g}',module.RatingParameters(surface_weight=surface,performance_time_scale=clock)) for surface in [0.25,0.45,0.65] for clock in [0,0.5,1] if (surface,clock)!=(0.45,1)]
    variants += [(f'k{scale:g}',module.RatingParameters(k_scale=scale)) for scale in [0.75,1.25]]
    records,score_frames,parameters = [],{},{}
    default_history = default_levels = None
    try:
        for index,(name,params) in enumerate(variants,1):
            print(f'[{index}/{len(variants)}] {name} 再計算開始',flush=True)
            started = time.time()
            with (out/f'{name}.log').open('w',encoding='utf-8') as log,contextlib.redirect_stdout(log):
                store = module.process_excel_to_memory(src,params)
            h,l,e = joined_frames(store)
            scores = per_race_metrics(h)
            score_frames[name] = scores
            parameters[name] = asdict(params)
            records.extend([{'candidate':name,**asdict(params),**row} for row in summarize_scores(scores)])
            if name=='default':
                default_history,default_levels = h,l
                h.to_pickle(out/'history.pkl')
                l.to_pickle(out/'levels.pkl')
                e.to_pickle(out/'entries.pkl')
                pd.DataFrame(store.races.values()).to_pickle(out/'races.pkl')
                pd.DataFrame(store.horse_rows.values()).to_pickle(out/'horses.pkl')
                # 現行の整合性検証も、実データの書き出し前の状態で実行する。
                integrity = {'horses':len(store.horses),'races':len(l),'entries':len(e),'actual_starts':len(h),'non_starters':int(e.status.isin(['取消','除外']).sum()),'max_zero_sum_error':float(h.groupby('race_id').adjusted_delta.sum().abs().max()),'non_finish_rating_changes':int((h.loc[~h.rating_updated,'adjusted_delta'].abs()>1e-9).sum())}
                (out/'integrity.json').write_text(json.dumps(integrity,ensure_ascii=False,indent=2),encoding='utf-8')
                records.extend([{'candidate':'market_pop',**row} for row in summarize_scores(per_race_metrics(h,'pop',True))])
            print(f'[{index}/{len(variants)}] {name} 完了 {time.time()-started:.1f}秒',flush=True)
    finally:
        pd.read_excel = reader
    frame = pd.DataFrame(records)
    frame.to_csv(out/'rating_metrics.csv',index=False,encoding='utf-8-sig')
    candidates = frame.loc[(frame.split=='validation')&(frame.candidate!='market_pop')].sort_values(['mean_spearman','candidate'],ascending=[False,True])
    selected = candidates.iloc[0].candidate
    candidate_test = score_frames[selected].loc[score_frames[selected].split=='test']
    baseline_test = score_frames['default'].loc[score_frames['default'].split=='test']
    rating_result = {'selected_by_validation':selected,'parameters':parameters[selected],'test_spearman_gain':bootstrap_gain(candidate_test,baseline_test,'spearman'),'test_win_gain':bootstrap_gain(candidate_test,baseline_test,'win')}
    (out/'selected_rating_parameters.json').write_text(json.dumps(parameters[selected],ensure_ascii=False,indent=2),encoding='utf-8')
    # レースレベルの重みはrating更新へ戻らないため、保存済み統計で独立比較する。
    level_frame,level_result = compare_levels(default_history,default_levels)
    level_frame.to_csv(out/'level_metrics.csv',index=False,encoding='utf-8-sig')
    result = {'train_end':TRAIN_END,'validation_end':VALID_END,'selection_rule':'rating: validation mean Spearman; level: validation next-start MSE','rating':rating_result,'level':level_result,'production_defaults_changed':False}
    (out/'selection.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    for path,before in hashes.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==before,path
    print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)


if __name__=='__main__':
    main()
