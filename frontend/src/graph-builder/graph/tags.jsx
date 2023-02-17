import React, { useState } from 'react';
import { render } from 'react-dom';
import { WithContext as ReactTags } from 'react-tag-input';

const suggestions = [
    { id: 'Tag_1', text: 'Tag_1' },
    { id: 'Tag_2', text: 'Tag_2' },
    { id: 'Tag_3', text: 'Tag_3' },
    { id: 'Tag_4', text: 'Tag_4' }
];

const KeyCodes = {
  comma: 188,
  enter: 13
};

const delimiters = [KeyCodes.comma, KeyCodes.enter];

export const Tags = () => {

  const [tags, setTags] = React.useState([
    { id: 'Tag_1', text: 'Tag_1' },
    { id: 'Tag_2', text: 'Tag_2' },
  ]);

  const handleDelete = i => {
    setTags(tags.filter((tag, index) => index !== i));
  };

  const handleAddition = tag => {
    setTags([...tags, tag]);
  };

  const handleDrag = (tag, currPos, newPos) => {
    const newTags = tags.slice();

    newTags.splice(currPos, 1);
    newTags.splice(newPos, 0, tag);

    // re-render
    setTags(newTags);
  };

  const handleTagClick = index => {
    console.log('The tag at index ' + index + ' was clicked');
  };

  return (
        <ReactTags
          tags={tags}
          suggestions={suggestions}
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
