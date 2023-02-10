import React, {useState, useMemo} from 'react';
import Header from './catalog/header/header';
import Catalog from './catalog/catalog';


const App = () => {
    const [content, setContent] = useState(<Catalog/>);
    const headerLinks = useMemo(() => [
        {
            'name': 'Catalog',
            'handleClick': () => {
                setContent(<Catalog/>)
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
        <div>
            {content}
        </div>
    </>
};

export default App;
