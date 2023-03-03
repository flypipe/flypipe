import React from "react";
import Select from "react-select";

const CustomSelect = ({
    id,
    name,
    onChange,
    options,
    value,
    className,
    isOptionDisabled,
}) => {
    const defaultValue = (options, value) => {
        return options ? options.find((option) => option.value === value) : "";
    };
    return (
        <div className={className}>
            <Select
                id={id}
                name={name}
                value={defaultValue(options, value)}
                onChange={(value) => onChange(value)}
                options={options}
                isOptionDisabled={isOptionDisabled}
            />
        </div>
    );
};

export default CustomSelect;
