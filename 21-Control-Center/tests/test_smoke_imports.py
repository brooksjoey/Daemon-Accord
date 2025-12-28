def test_control_center_imports():
    # Make the test runnable without packaging by adding `src/` to sys.path.
    import sys
    from pathlib import Path

    module_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(module_root / "src"))

    from control_center import create_app  # noqa: E402

    app = create_app()
    assert app.name == "control-center"

