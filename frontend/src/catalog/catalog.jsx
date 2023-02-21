import React, { useState, useCallback } from "react";
import Search from "./search/search";
import SearchResults from "./search-results/search-results";

const Catalog = () => {
    const [searchResultNodes, setSearchResultNodes] = useState(nodes);
    const handleUpdateSearch = useCallback((results) => {
        setSearchResultNodes(results);
    }, []);

    return (
        <>
            <div className="d-flex justify-content-around h-75">
                <Search nodes={nodes} handleUpdateSearch={handleUpdateSearch} />
                <SearchResults nodes={searchResultNodes} />
            </div>
        </>
    );
};

export default Catalog;
