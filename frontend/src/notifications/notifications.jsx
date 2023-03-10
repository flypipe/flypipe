import React, { useState, useEffect } from "react";
import Toast from "react-bootstrap/Toast";
import ToastContainer from "react-bootstrap/ToastContainer";
import ToastHeader from "react-bootstrap/ToastHeader";
import ToastBody from "react-bootstrap/ToastBody";

const DELAY_MILLISECONDS = 3000;

/*
Stack of toast notifications. These notifications quietly die after DELAY_MILLISECONDS, which gave me a design 
conundrum: 
- I didn't want to store the list of currently active messages on the parent component as it would require both a new 
state + since removal is time based and managed by this component we'd need a new callback for the parent to pass here 
for the notifications component to invoke to clean up removed entries from the list, this would clutter up the parent 
when the purpose of this toast notifications is to give a cheap and simple throwaway notification message. 
- We can avoid this state management by passing the message we want to the notifications component and having a 
useEffect that kicks off whenever this changes to append it to a list of messages in state. However, if we pass the 
same message twice then the toast notification will only activate once, because if the prop doesn't change then the 
Notifications component won't rerender. 
- The solution is to have a message id as well that is just incremented by the parent whenever it wants to issue a 
new toast message. This ensures that even if the message is repeated a second time it will still have a different 
msgId and will prompt a rerender. 
*/
const Notifications = ({ newMessage }) => {
    const [messages, setMessages] = useState([]);
    useEffect(() => {
        if (newMessage.message) {
            setMessages((prevMessages) => [...prevMessages, newMessage]);
        }
    }, [newMessage]);
    return (
        <ToastContainer position="top-end" className="m-4">
            {messages.map(({ msgId, message }) => (
                <Toast
                    key={`toast-msg-${msgId}`}
                    onClose={() => {
                        setMessages((prevMessages) => {
                            const nextMessages = [...prevMessages];
                            nextMessages.splice(
                                nextMessages.findIndex(
                                    (message) => message.msgId === msgId
                                ),
                                1
                            );
                            return nextMessages;
                        });
                    }}
                    delay={DELAY_MILLISECONDS}
                    autohide
                >
                    {/* <ToastHeader></ToastHeader> */}
                    <ToastBody>{message}</ToastBody>
                </Toast>
            ))}
        </ToastContainer>
    );
};

export default Notifications;
