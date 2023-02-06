import React from 'react';
import classNames from 'classnames';


const NavTab = ({title, isSelected, onClick, ...props}) => {
    // Using classNames in order to have conditional classes (https://stackoverflow.com/a/35718621)
    const linkClasses = classNames(
        "nav-link",
        "active",
        {
            "border-bottom": isSelected,
            "fw-bold": isSelected,
        }
    );
    return <li className="nav-item">
        <a className={linkClasses} onClick={onClick} {...props}>{title}</a>
    </li>
};

export default NavTab;
