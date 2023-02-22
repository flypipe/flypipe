import React, { useState } from "react";
import { render } from "react-dom";
import { WithContext as ReactTags } from "react-tag-input";

const KeyCodes = {
    comma: 188,
    enter: 13,
};

const delimiters = [KeyCodes.comma, KeyCodes.enter];

export const Tags = ({ tags: initialTags, tagSuggestions, onSetTags }) => {
    const [tags, setTags] = useState(initialTags);
    const handleDelete = (index) => {
        const newTags = tags.filter(({}, i) => i !== index);
        setTags(newTags);
        onSetTags(newTags);
    };

    const handleAddition = (tag) => {
        const newTags = [...tags, tag];
        setTags(newTags);
        onSetTags(newTags);
    };

    const handleDrag = (tag, currPos, newPos) => {
        const newTags = tags.slice();

        newTags.splice(currPos, 1);
        newTags.splice(newPos, 0, tag);

        setTags(newTags);
        onSetTags(newTags);
    };

    return (
        <ReactTags
            id="tags"
            name="tags"
            tags={tags}
            suggestions={tagSuggestions}
            delimiters={delimiters}
            handleDelete={handleDelete}
            handleAddition={handleAddition}
            handleDrag={handleDrag}
            inputFieldPosition="top"
            autocomplete
            classNames={{
                tagInputField: "form-control",
                selected: "selectedClass mt-2",
                remove: "btn btn-light btn-sm",
                suggestions: "suggestionsClass",
            }}
        />
    );
};
