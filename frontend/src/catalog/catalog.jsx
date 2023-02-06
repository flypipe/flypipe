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
        "name": "country_code2",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code3",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code4",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code5",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code6",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code7",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code8",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code9",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code10",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code11",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code12",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code13",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code14",
        "filePath": "this/is/the/path/to/country/code.py",
        "description": "This is the description of the country code",
        "tags": ["feature", "test"],
        "schema": [],
        "predecessors": [],
        "successors": ["dummy_1"],
    },
    {
        "key": "country_code_1",
        "name": "country_code15",
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