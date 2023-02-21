import React, { useState, useContext } from "react";
import GraphBuilder from "./graph-builder/graph-builder";
import Notifications from "./catalog/notifications";
import { NotificationContext } from "./context";


const App = () => {
    const [content, setContent] = useState(<GraphBuilder />);
    const { newMessage, setNewMessage } = useContext(NotificationContext);
    
    return (
        <>
            <Notifications newMessage={newMessage} />
            <div className="d-flex w-100 h-100">{content}</div>
        </>
    );
};

export default App;
