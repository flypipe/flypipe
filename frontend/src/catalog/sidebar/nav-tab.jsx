import React from 'react';


const NavTab = ({title, isSelected, onClick, ...props}) => {
    
    return <li className="nav-item">
        <a className="nav-link active" onClick={onClick} {...props}>{title}{isSelected ? '!' : ''}</a>
    </li>
};

export default NavTab;
