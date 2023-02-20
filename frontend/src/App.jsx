import React, {useState, useMemo} from 'react';
import Header from './header/header';
import GraphBuilder from './graph-builder/graph-builder';


const App = () => {
    const [content, setContent] = useState(<GraphBuilder/>);
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
        <div>
            {content}
        </div>
    </>
};

export default App;
