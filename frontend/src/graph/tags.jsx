import React, { useState } from "react";
import { render } from "react-dom";
import { WithContext as ReactTags } from "react-tag-input";

const KeyCodes = {
    comma: 188,
    enter: 13,
};

const delimiters = [KeyCodes.comma, KeyCodes.enter];

export const Tags = ({ tags, tagSuggestions, onSetTags }) => {
    const handleDelete = (index) => {
        const newTags = tags.filter(({}, i) => i !== index);
        onSetTags(newTags);
    };

    const handleAddition = (tag) => {
        const newTags = [...tags, tag];
        onSetTags(newTags);
    };

    const handleDrag = (tag, currPos, newPos) => {
        const newTags = tags.slice();

        newTags.splice(currPos, 1);
        newTags.splice(newPos, 0, tag);

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
