import React, {useState, useCallback, useRef, useEffect} from 'react';


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


const Search = ({nodes, handleUpdateSearch}) => {
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
        {
            'name': 'Schema',
            'value': 'schema'
        },
        {
            'name': 'Predecessors',
            'value': 'predecessors'
        },
        {
            'name': 'Successors',
            'value': 'successors'
        },
    ];
    const [searchText, setSearchText] = useState('');
    const [selectedSearchOptions, setSelectedSearchOptions] = useState(allSearchOptions.map(({value}) => value));
    const onUpdateSearch = useCallback(() => {
        const searchText = searchTextRef.current.value;
        const results = searchNodes(searchText, nodes, selectedSearchOptions);
        handleUpdateSearch(results);
    }, [nodes, selectedSearchOptions])
    useEffect(() => {
        onUpdateSearch();
    }, [nodes, searchText, selectedSearchOptions]);
    const searchTextRef = useRef(null);
    return <div className="d-flex flex-column col-2 m-4">
        <div className="form-outline">
          <input type="search" className="form-control" ref={searchTextRef} placeholder="Search" aria-label="Search" onChange={(e) => {setSearchText(e.target.value)}} />
        </div>
        <br/>
        <h5>Search By</h5>
        {allSearchOptions.map(({name, value}, i) => 
            <div className="d-flex p-1" key={`search_filter_${i}`}>
                <input id={`search_${value}`} className="form-check-input mx-2" type="checkbox" name="search_type" defaultChecked value={value} onChange={(e) => {
                    const searchValue = e.target.value;
                    if (selectedSearchOptions.includes(searchValue)) {
                        setSelectedSearchOptions((prevState) => {
                            const newSelectedSearchOptions = [...prevState]
                            newSelectedSearchOptions.splice(newSelectedSearchOptions.indexOf(searchValue), 1);
                            return newSelectedSearchOptions
                        });
                    } else {
                        setSelectedSearchOptions((prevState) => {
                            return [...prevState, searchValue]
                        });
                    }
                }}/>
                <label className="form-check-label" htmlFor="search_name">{name}</label>
            </div>
        )}
    </div>
}

export default Search;