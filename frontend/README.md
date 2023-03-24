# Flypipe Frontend

This subproject contains the code for Flypipe's frontend. As of the current version this is the code for the Catalog UI, seperately in the main Python project there is also some frontend code that handles rendering a node pipeline's graph which will shortly be moved here.

For convenience sake we aren't publishing this as a standalone package to npm. The main project readme is [here](../README.md).

## Installation and Execution

-   Install nodejs (https://nodejs.org/en/download/)
-   Install yarn `npm install --global yarn`
-   Navigate in a terminal to `flypipe/frontend` and run `yarn install` to install the dependencies the project requires.
-   Run either `yarn start` to run the frontend standalone on a development server or `yarn build` to build the javascript artifacts. Note that `yarn build` also copies the built artifact (`frontend/dist/bundle.js`) over to the Python project (`flypipe/js/bundle.js`) which is necessary for the Flypipe project to be able to use the frontend code.
