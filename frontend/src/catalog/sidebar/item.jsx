import React, {useState} from 'react';

const Item = ({title, isSelected, onClick, ...props}) => {
    return <li className="nav-item">
        <a className="nav-link active" onClick={() => {onClick()}} {...props}>{title}{isSelected ? '!' : ''}</a>
    </li>
};

export default Item;