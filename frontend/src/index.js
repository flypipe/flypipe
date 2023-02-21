// Import Bootstrap css
import "bootstrap/dist/css/bootstrap.min.css";
import "./scss/styles.scss";

import React from "react";
import ReactDOM from "react-dom";
import App from "./App";
import { NotificationContextProvider } from "./context";
ReactDOM.render(
    <NotificationContextProvider>
        <App />
    </NotificationContextProvider>,
    document.getElementById("root")
);
