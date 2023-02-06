import React, {useState} from 'react';
import NavTabs from './nav-tabs';
import SidebarDetails from './sidebar-details';
import NodeSearchResults from './node-search-results';


const SideBar = () => {
    const navItemDefs = [
        {
            key: 'nav-key-1',
            title: "Results",
            details: <NodeSearchResults/>,
        },
        {
            key: 'nav-key-2',
            title: "Node Builder",
            details: <p>{'<Node Builder>'}</p>
        }
    ]
    const [selectedDetails, setSelectedDetails] = useState(navItemDefs[0].details);
    // const [currentDetails, setCurrentDetails] = useState(navItemDefs[0].details);
    const handleChangeTab = (details) => {
        setSelectedDetails(details);
    };
    return <div className="d-flex flex-column col-2">
        <NavTabs className="d-flex flex-row" tabDefs={navItemDefs} handleTabClick={handleChangeTab}/>
        <SidebarDetails className={{flexGrow: 2}}>
            {selectedDetails}
        </SidebarDetails>
    </div>;
}

export default SideBar;

// const NavTab = () => {
//     return <NavItem key={key} title={title} isSelected={key == selectedNav} onClick={onClick}></NavItem>
// }
// () => {
//     setTabContent(component);
//     setSelectedNav(key);
// }