import React from "react";
import Select from 'react-select';

export default ({ id, name, onChange, options, value, className }) => {
    const defaultValue = (options, value) => {
        return options ? options.find(option=>option.value === value): "";
    }
    return (
        <div className={className}>
            <Select
                id={id}
                name={name}
                value={defaultValue(options, value)}
                onChange={ value=> onChange(value)}
                options={options}/>
        </div>
    );
}