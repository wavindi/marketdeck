import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="MarketDeck local workstation")
    parser.add_argument("command", choices=["serve", "tui"])
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn
        uvicorn.run("marketdeck.api:app", host="127.0.0.1", port=8765, reload=False)
    else:
        from .tui import main as tui_main
        tui_main()


if __name__ == "__main__":
    main()
