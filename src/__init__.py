"""FACS - CWMP/TR-069 Fast Auto Configuration Server"""

__version__ = "0.1.0"


def cli() -> int:
    """Entry point for the FACS ACS server."""
    from src.main import main

    return main()


__all__ = ["cli"]
