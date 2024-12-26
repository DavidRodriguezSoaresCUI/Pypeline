from pathlib import Path


def main() -> None:
    conf_file = Path('./source/conf.py')
    conf = conf_file.read_text(encoding='utf8')
    if not 'sys.path.insert' in conf:
        conf_file.write_text(
            "import os\nimport sys\nsys.path.insert(0, os.path.abspath('../../src/'))\n"
            + conf.replace("html_theme = 'alabaster'", "html_theme = 'furo'"),
            encoding='utf8'
        )


if __name__ == '__main__':
    main()
