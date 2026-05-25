def build_dbt_command(subcommand: str) -> str:
    return (
        f"$DBT_VENV_PATH/bin/dbt {subcommand} "
        "--project-dir $DBT_PROJECT_DIR "
        "--profiles-dir $DBT_PROFILES_DIR "
        "--target $DBT_TARGET"
    )
