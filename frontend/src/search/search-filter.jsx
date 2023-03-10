import React, { useCallback } from "react";

const SearchFilter = ({ name, title, defaultChecked, handleChangeFilter }) => {
    const onChange = useCallback(
        (e) => {
            const value = e.target.checked;
            handleChangeFilter(name, value);
        },
        [name]
    );
    const id = `filter-${name}`;
    return (
        <div className="mx-2">
            <input
                type="checkbox"
                id={id}
                className="form-check-input mx-2"
                defaultChecked={defaultChecked}
                value={title}
                onChange={onChange}
            />
            <label htmlFor={id}>{title}</label>
        </div>
    );
};

export default SearchFilter;
