import React, {useState, useCallback} from 'react';
import SideBar from './sidebar/sidebar';
import Details from './details';
import SearchBar from './searchbar/searchbar';

// TODO: this should be supplied by the frontend
const nodes = [
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
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
        "description": "<dummy>",
        "tags": ["test"],
        "schema": [],
        "predecessors": ["country_code_1"],
        "successors": [],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
]

const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);
    return <div className="d-flex">
        <SideBar nodes={searchResultNodes}/>
        <Details/>
        <SearchBar nodes={nodes} handleUpdateSearch={handleUpdateSearch}/>
    </div>
}

export default Catalog;