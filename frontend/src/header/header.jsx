import React, {useCallback, useState} from 'react';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import HeaderLink from './header-link';


const Header = ({links}) => {
    const getLinkKey = useCallback((i) => `header-link-${i}`, []);
    const [selectedLink, setSelectedLink] = useState(getLinkKey(0));
    const getLinkHandleClick = useCallback((key, linkHandleClick) => () => {
        setSelectedLink(key);
        linkHandleClick();
    }, []);
    return <Navbar className="px-4" bg="info" variant="white">
        {/* <Container> */}
            <Navbar.Brand>Flypipe</Navbar.Brand>
            <Navbar.Toggle aria-controls="basic-navbar-nav" />
            <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="me-auto">
                {links.map(({name, handleClick}, i) => {
                    const key = getLinkKey(i);
                    return <HeaderLink key={key} selected={selectedLink === key} name={name} handleClick={getLinkHandleClick(key, handleClick)}/>
                })}
            </Nav>
            </Navbar.Collapse>
        {/* </Container> */}
    </Navbar>
}

export default Header;