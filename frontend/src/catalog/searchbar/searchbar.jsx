import React from 'react';


const SearchBar = ({nodes}) => {
    return <div className="d-flex flex-column col">
        <h2>Search</h2>
        <input id="search_name" type="radio" name="search_type" value="name"/>
        <label htmlFor="search_name">Name</label>
        <input id="search_description" type="radio" name="search_type" value="description"/>
        <label htmlFor="search_description">Description</label>
        <input id="search_tags" type="radio" name="search_type" value="tags"/>
        <label htmlFor="search_tags">Tags</label>
    </div>
}

export default SearchBar;