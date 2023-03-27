import React, { useState } from "react";
import ReactTags from "react-tag-autocomplete";
import "../../styles/tag-styles.css";

const delimiters = ["Comma", "Enter"];

export const Tags = ({ tags, tagSuggestions, onSetTags }) => {
    const handleDelete = (index) => {
        const newTags = tags.filter(({}, i) => i !== index);
        onSetTags(newTags);
    };

    const handleAddition = (tag) => {
        const newTags = [...tags, tag];
        onSetTags(newTags);
    };

    return (
        <ReactTags
            id="tags"
            name="tags"
            tags={tags}
            allowNew={true}
            placeholderText="Tags"
            suggestions={tagSuggestions}
            delimiters={delimiters}
            onDelete={handleDelete}
            onAddition={handleAddition}
            // inputFieldPosition="top"
            // autocomplete
            classNames={{
                tagInputField: "form-control",
                selected: "selectedClass mt-2",
                remove: "btn btn-light btn-sm",
                suggestions: "suggestionsClass",
            }}
        />
    );
};
