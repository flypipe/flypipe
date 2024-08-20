// Import Bootstrap css
import "bootstrap/dist/css/bootstrap.min.css";
import "../styles/styles.scss";

import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { NotificationContextProvider } from "./notifications/context";
const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(
    <NotificationContextProvider>
        <App />
    </NotificationContextProvider>
);
