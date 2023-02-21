
import React, { useState } from "react";
import uuid from 'react-uuid';

export const NotificationContext = React.createContext();

export const NotificationContextProvider = ({ children }) => {
	const [newMessage, setNewMessage] = useState({msgId: uuid(), message: ""});

	return (
		<NotificationContext.Provider value={{ newMessage, setNewMessage }}>
			{children}
		</NotificationContext.Provider>
	);
};
