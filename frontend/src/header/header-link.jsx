import React from "react";
import Nav from "react-bootstrap/Nav";
import classNames from "classnames";

const HeaderLink = ({ name, handleClick, selected, ...props }) => {
    return (
        <Nav.Link
            className={classNames({
                "fw-bold": selected,
            })}
            onClick={handleClick}
            {...props}
        >
            {name}
        </Nav.Link>
    );
};

export default HeaderLink;
