import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.api.v1.deps import get_db
from app.core.config import get_settings
from app.schemas.chatgpt_manual import (
    ChatgptManualHistoryRead,
    ChatgptPromptGenerateRequest,
    ChatgptPromptRead,
    ChatgptResponseSaveRequest,
)
from app.services.chatgpt_manual import (
    ChatgptManualError,
    generate_chatgpt_prompt,
    list_chatgpt_history,
    save_chatgpt_response,
)


LOGGER = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/chatgpt/prompts",
    response_model=ChatgptPromptRead,
    status_code=status.HTTP_201_CREATED,
)
def create_chatgpt_prompt(
    payload: ChatgptPromptGenerateRequest,
    db: Session = Depends(get_db),
) -> ChatgptPromptRead:
    """Create an editable prompt without sending race data to ChatGPT."""

    settings = get_settings()
    try:
        record = generate_chatgpt_prompt(db, payload.race_id, settings=settings)
    except ChatgptManualError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        LOGGER.exception("ChatGPT prompt generation failed: %s", exc.__class__.__name__)
        raise HTTPException(
            status_code=500,
            detail="ChatGPT用プロンプトの生成に失敗しました",
        ) from exc

    prompt_length = len(record.prompt_text)
    return ChatgptPromptRead(
        history_id=record.id,
        race_id=record.race_id,
        prompt_text=record.prompt_text,
        prompt_length=prompt_length,
        warning_threshold=settings.chatgpt_prompt_length_warning,
        length_warning=prompt_length > settings.chatgpt_prompt_length_warning,
        chatgpt_url=settings.chatgpt_url,
    )


@router.post(
    "/chatgpt/responses",
    response_model=ChatgptManualHistoryRead,
    status_code=status.HTTP_201_CREATED,
)
def create_chatgpt_response(
    payload: ChatgptResponseSaveRequest,
    db: Session = Depends(get_db),
):
    """Persist a response pasted manually by the user."""

    try:
        return save_chatgpt_response(
            db,
            race_id=payload.race_id,
            prompt_text=payload.prompt_text,
            response_text=payload.response_text,
            history_id=payload.history_id,
        )
    except ChatgptManualError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        LOGGER.exception("ChatGPT response save failed: %s", exc.__class__.__name__)
        raise HTTPException(
            status_code=500,
            detail="ChatGPT予想結果の保存に失敗しました",
        ) from exc


@router.get(
    "/races/{race_id}/chatgpt-predictions",
    response_model=list[ChatgptManualHistoryRead],
)
def get_chatgpt_history(
    race_id: str,
    db: Session = Depends(get_db),
    limit: int = Query(default=20, ge=1, le=100),
):
    """Load manual prompt/response history for one race."""

    try:
        return list_chatgpt_history(db, race_id, limit=limit)
    except Exception as exc:
        LOGGER.exception("ChatGPT history load failed: %s", exc.__class__.__name__)
        raise HTTPException(
            status_code=500,
            detail="過去のChatGPT予想履歴を読み込めませんでした",
        ) from exc
