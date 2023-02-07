import React, {useState, useCallback} from 'react';
import SideBar from './sidebar/sidebar';
import Details from './details';
import SearchBar from './searchbar/searchbar';
import Header from './header';

// TODO: this should be supplied by the frontend
const nodes = [
    {
        "key": "country_code_1",
        "name": "country_code1",
        "filePath": "this/is/the/path/to/country/code.py",
        "importCmd": "from this.is.the.path.to.country.code import Code",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "dummy_1",
        "name": "dummy",
        "filePath": "bla/dummy.py",
        "importCmd": "from bla.dummy import Dummy",
        "description": "<dummy>",
        "tags": ["test"],
        "schema": [],
        "predecessors": ["country_code_1"],
        "successors": [],
    },
]

const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);
    return <>
        <Header/>
        <div className="d-flex">
            <SideBar nodes={searchResultNodes}/>
            <Details/>
            <SearchBar nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
        </div>
    </>
}

export default Catalog;