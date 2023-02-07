import React from 'react';
import Container from 'react-bootstrap/Container';
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import NavDropdown from 'react-bootstrap/NavDropdown';


const Header = () => {
    return <Navbar className="px-4" bg="info" variant="dark">
        {/* <Container> */}
            <Navbar.Brand>Flypipe Catalog</Navbar.Brand>
            <Navbar.Toggle aria-controls="basic-navbar-nav" />
            <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="me-auto">
                <Nav.Link href="//flypipe.github.io/flypipe/">Documentation</Nav.Link>
                {/* <NavDropdown title="Actions" id="actions-nav-dropdown">
                </NavDropdown> */}
            </Nav>
            </Navbar.Collapse>
        {/* </Container> */}
    </Navbar>
}

export default Header;