import React, { useState } from 'react';
import { render } from 'react-dom';
import { WithContext as ReactTags } from 'react-tag-input';

const KeyCodes = {
  comma: 188,
  enter: 13
};

const delimiters = [KeyCodes.comma, KeyCodes.enter];

export const Tags = ( { formik, tagsSuggestions } ) => {

  
  const handleDelete = i => {
    const newTags = formik.values.tags.filter((tag, index) => index !== i)
    formik.setFieldValue('tags', newTags);
  };

  const handleAddition = tag => {
    const newTags = [...formik.values.tags, tag];
    console.log("tags=>", newTags);
    formik.setFieldValue('tags', newTags);
  };

  const handleDrag = (tag, currPos, newPos) => {
    const newTags = formik.values.tags.slice();

    newTags.splice(currPos, 1);
    newTags.splice(newPos, 0, tag);

    // re-render
    formik.setFieldValue('tags', newTags);
  };

  const handleTagClick = index => {
    console.log('The tag at index ' + index + ' was clicked');
  };

  return (
        <ReactTags
          id="tags"
          name="tags"
          tags={formik.values.tags}
          suggestions={tagsSuggestions}
          delimiters={delimiters}
          handleDelete={handleDelete}
          handleAddition={handleAddition}
          handleDrag={handleDrag}
          handleTagClick={handleTagClick}
          inputFieldPosition="top"
          autocomplete
          classNames={{
            tagInputField: 'form-control',
            selected: 'selectedClass mt-2',
            remove: 'btn btn-light btn-sm',
            suggestions: 'suggestionsClass',            
          }}
        />    
  );
};
