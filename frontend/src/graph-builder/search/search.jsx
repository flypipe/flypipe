import React, {
    useMemo,
    useState,
    useCallback,
    useEffect,
    useRef,
} from "react";
import { BsFilter } from "react-icons/bs";
import NodeResultsList from "./node-results-list";
import Fuse from "fuse.js";
import Dropdown from "react-bootstrap/Dropdown";
import SearchFilter from "./search-filter";

const filterDefs = [
    {
        name: "name",
        title: "Name",
    },
    {
        name: "description",
        title: "Description",
    },
    {
        name: "output.column",
        title: "Schema",
    },
    {
        name: "tags.text",
        title: "Tags",
    },
];

const search = (fuse, searchString) => {
    if (!searchString) {
        return nodes;
    } else {
        const rawSearchResults = fuse.search(searchString);
        return rawSearchResults.map(({ item }) => item);
    }
};

const Search = ({ nodes }) => {
    const [filters, setFilters] = useState(filterDefs.map(({ name }) => name));
    const fuse = useMemo(
        () =>
            new Fuse(nodes, {
                keys: filters,
            }),
        [nodes, filters]
    );
    const [searchResults, setSearchResults] = useState(nodes);
    const numberSearchResultsText = useMemo(
        () =>
            searchResults.length === 1
                ? `${searchResults.length} Result`
                : `${searchResults.length} Results`,
        [searchResults]
    );
    const searchInput = useRef(null);
    const onSearchChange = useCallback(
        (e) => {
            const results = search(fuse, e.target.value);
            setSearchResults(results);
        },
        [fuse, setSearchResults]
    );
    useEffect(() => {
        if (!searchInput) {
            return;
        }
        const results = search(fuse, searchInput.current.value);
        setSearchResults(results);
    }, [searchInput, fuse, setSearchResults]);

    const handleChangeFilter = useCallback(
        (name, value) => {
            if (value) {
                setFilters((prevState) => [...prevState, name]);
            } else {
                setFilters((prevState) =>
                    prevState.reduce((accumulator, currentValue) => {
                        if (currentValue === name) {
                            return accumulator;
                        } else {
                            return [...accumulator, currentValue];
                        }
                    }, [])
                );
            }
        },
        [setFilters]
    );

    return (
        <div id="search" className="search-result">
            <div className="form-outline p-2">
                <input
                    type="search"
                    className="form-control"
                    placeholder="Search"
                    aria-label="Search"
                    onChange={onSearchChange}
                    ref={searchInput}
                />
            </div>
            <br />
            <div className="d-flex justify-content-between align-items-center">
                <Dropdown>
                    <Dropdown.Toggle
                        variant="light flypipe"
                        className="no-border"
                    >
                        <BsFilter size={21} />
                        All filters
                    </Dropdown.Toggle>

                    <Dropdown.Menu>
                        <Dropdown.Header>Filter By</Dropdown.Header>
                        {filterDefs.map(({ name, title }) => (
                            <SearchFilter
                                key={name}
                                name={name}
                                title={title}
                                defaultChecked={filters.includes(name)}
                                handleChangeFilter={handleChangeFilter}
                            />
                        ))}
                        {/* <Dropdown.Header>Group</Dropdown.Header> */}
                    </Dropdown.Menu>
                </Dropdown>
                <h6 className="mb-0 me-2">{numberSearchResultsText}</h6>
            </div>
            <NodeResultsList nodes={searchResults} allNodes={nodes} />
        </div>
    );
};

export default Search;
