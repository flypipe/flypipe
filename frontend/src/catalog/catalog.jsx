import React, {useState, useCallback} from 'react';
import Search from './search/search';
import SearchResults from './search-results/search-results';
import Notifications from './notifications';


let i = 1;

const Catalog = () => {
    const [newMessage, setNewMessage] = useState(null);
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);

    return <>
        
        <div className="d-flex justify-content-around h-75">
        
            {/* <Search nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
            <SearchResults nodes={searchResultNodes}/> */}
            <button onClick={() => {
            i += 1;
            setNewMessage({
                msgId: i,
                message: "Hello"
            })
        }}>NOTIFICATION</button>
            <Notifications newMessage={newMessage}/>
        </div>
    </>
}

export default Catalog;