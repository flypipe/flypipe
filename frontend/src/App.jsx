import React, {useState, useMemo, useContext} from 'react';
import Header from './header/header';
import GraphBuilder from './graph-builder/graph-builder';
import Notifications from './catalog/notifications';
import { NotificationContext } from './context';
import uuid from 'react-uuid';



const App = () => {
    const [content, setContent] = useState(<GraphBuilder/>);
    const { newMessage, setNewMessage } = useContext(NotificationContext);
    const headerLinks = useMemo(() => [
        {
            'name': 'Graph Builder',
            'handleClick': () => {
                setContent(<GraphBuilder/>)
            }
        },
        {
            'name': 'Documentation',
            'handleClick': () => {
                window.location.href = '//flypipe.github.io/flypipe/';
            }
        }
    ], []);
    return <>
        
            <Header links={headerLinks}/>
            
            <button onClick={() => {
                
                console.log(uuid());
                setNewMessage({
                    msgId: uuid(),
                    message: "Hello"
                })
            }}>NOTIFICATION</button>
            
            <Notifications newMessage={newMessage}/>
            
            <div>
                {content}
            </div>
    </>
};

export default App;
