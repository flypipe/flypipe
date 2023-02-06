import React, {useState, useCallback, useRef} from 'react';


const isNodeInSearch = (searchText, node, options) => {
    for (const option of options) {
        // If the option field is an array then we need to dig into each element in the array to check if the searchText is a substring. 
        if (Array.isArray(node[option])) {
            for (const elem of node[option]) {
                if (elem.includes(searchText)) {
                    return true;
                }
            }
        } else {
            if (node[option].includes(searchText)) {
                return true
            }
        }
    }
    return false;
}


const searchNodes = (searchText, nodes, options) => {
    const results = [];
    for (const node of nodes) {
        if (isNodeInSearch(searchText, node, options)) {
            results.push(node);
        }
    }
    return results;
};


const SearchBar = ({nodes, handleUpdateSearch}) => {
    const allSearchOptions = [
        {
            'name': 'Name',
            'value': 'name'
        },
        {
            'name': 'Description',
            'value': 'description'
        },
        {
            'name': 'Tags',
            'value': 'tags'
        },
    ];
    const [selectedSearchOptions, setSelectedSearchOptions] = useState(allSearchOptions.map(({value}) => value));
    const onUpdateSearch = useCallback(() => {
        const searchText = searchTextRef.current.value;
        const results = searchNodes(searchText, nodes, selectedSearchOptions);
        handleUpdateSearch(results);
    }, [nodes, selectedSearchOptions])
    const searchTextRef = useRef(null);
    return <div className="d-flex flex-column col">
        <h2>Search</h2>
        <div className="form-outline">
          <input type="search" className="form-control" ref={searchTextRef} placeholder="Type query" aria-label="Search" onChange={onUpdateSearch} />
        </div>
        {allSearchOptions.map(({name, value}, i) => 
            <div className="d-flex" key={`search_filter_${i}`}>
                <input id={`search_${value}`} className="form-check-input" type="checkbox" name="search_type" defaultChecked value={value} onChange={(e) => {
                    const searchValue = e.target.value;
                    if (selectedSearchOptions.includes(searchValue)) {
                        const newSelectedSearchOptions = [...selectedSearchOptions]
                        newSelectedSearchOptions.splice(newSelectedSearchOptions.indexOf(searchValue), 1);
                        setSelectedSearchOptions(newSelectedSearchOptions);
                    } else {
                        setSelectedSearchOptions([...selectedSearchOptions, searchValue]);
                    }
                    onUpdateSearch();
                }}/>
                <label className="form-check-label" htmlFor="search_name">{name}</label>
            </div>
        )}
    </div>
}

export default SearchBar;