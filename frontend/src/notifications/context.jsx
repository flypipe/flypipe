import React, { useState, useCallback } from "react";
import uuid from "react-uuid";

export const NotificationContext = React.createContext();

export const NotificationContextProvider = ({ children }) => {
    const [newMessage, setNewMessage] = useState({
        msgId: uuid(),
        message: "",
    });
    const addNotification = useCallback(
        (message) => {
            setNewMessage({
                msgId: uuid(),
                message,
            });
        },
        [setNewMessage, uuid]
    );

    return (
        <NotificationContext.Provider
            value={{ newMessage, setNewMessage, addNotification }}
        >
            {children}
        </NotificationContext.Provider>
    );
};
