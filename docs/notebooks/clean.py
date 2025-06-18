import os
import pathlib

import nbformat


def clean_notebook(notebook_path):
    """
    Cleans a single .ipynb file: removes output cells and resets execution counts.
    """
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=nbformat.NO_CONVERT)

        changed = False
        for cell in nb.cells:
            if cell.cell_type == 'code':
                if cell.get('outputs'):
                    cell['outputs'] = []
                    changed = True
                if cell.get('execution_count') is not None:
                    cell['execution_count'] = None
                    changed = True

        if changed:
            with open(notebook_path, 'w', encoding='utf-8') as f:
                nbformat.write(nb, f)
            print(f"Cleaned: {notebook_path}")
        else:
            print(f"Already clean: {notebook_path}")
    except Exception as e:
        print(f"Error processing {notebook_path}: {e}")


def clean_all_notebooks(root_dir):
    """
    Recursively finds and cleans all .ipynb files starting from root_dir.
    """
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.ipynb'):
                notebook_path = os.path.join(dirpath, filename)
                clean_notebook(notebook_path)


if __name__ == "__main__":
    clean_all_notebooks(os.path.join(pathlib.Path(__file__).resolve().parent))
