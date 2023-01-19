from sys import argv

import toml

if len(argv) != 2:
    raise TypeError(
        "Expected set_version to be invoked with a single argument <version>"
    )

version = argv[1]
project_data = toml.load("pyproject.toml")
project_data["project"]["version"] = version
with open("pyproject.toml", "w") as f:
    toml.dump(project_data, f)
