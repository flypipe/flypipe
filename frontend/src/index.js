// Import Bootstrap css
import "bootstrap/dist/css/bootstrap.min.css";
import "../styles/styles.scss";

import React from "react";
import ReactDOM from "react-dom";
import App from "./App";
import { NotificationContextProvider } from "./notifications/context";
ReactDOM.render(
    <NotificationContextProvider>
        <App />
    </NotificationContextProvider>,
    document.getElementById("root")
);
