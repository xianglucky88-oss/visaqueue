
import json, sys
from . import pipeline
from .history import document_history, progress_report, visible_snapshot
from .store import Store


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"

    if argv and argv[0] == "history":
        return _history(argv[1:])
    if argv and argv[0] == "progress":
        con = Store().connect()
        try:
            print(json.dumps(progress_report(Store(con)), ensure_ascii=False, indent=2))
        finally:
            con.close()
        return 0

    print(json.dumps(pipeline.run(mode=mode), ensure_ascii=False))
    return 0


def _history(args):
    as_of = None
    if "--as-of" in args:
        i = args.index("--as-of")
        as_of = args[i + 1]
        args = args[:i] + args[i + 2:]
    con = Store().connect()
    try:
        store = Store(con)
        if args and args[0] == "document" and len(args) >= 3:
            result = document_history(store, args[1], args[2], as_of=as_of)
        else:
            result = visible_snapshot(store, as_of=as_of)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
