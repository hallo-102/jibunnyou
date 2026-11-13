"""PyInstallerと通常のPythonで共用する画面起動入口。"""
from desktop_monitor.app import main


if __name__ == "__main__":
    # ビルド時と開発時に同じアプリケーションを起動する。
    main()
