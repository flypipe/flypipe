import React, {useState, useMemo} from 'react';
import NavTabs from './nav-tabs';
import SidebarDetails from './sidebar-details';
import NodeSearchResults from './node-search-results';


const SideBar = ({nodes}) => {
    const ResultsElement = useMemo(() => <NodeSearchResults nodes={nodes}/>, [nodes]);
    const navItemDefs = [
        {
            key: 'nav-key-1',
            title: "Results",
            details: ResultsElement,
        },
        {
            key: 'nav-key-2',
            title: "Node Builder",
            // TODO: add node builder tag here...
            details: <p>{'<Node Builder>'}</p>
        }
    ];
    const [selectedDetailsKey, setSelectedDetailsKey] = useState(navItemDefs[0].key);
    // const [currentDetails, setCurrentDetails] = useState(navItemDefs[0].details);
    const handleChangeTab = (key) => {
        setSelectedDetailsKey(key);
    };
    
    return <div className="d-flex flex-column col-2">
        <NavTabs className="d-flex flex-row" tabDefs={navItemDefs} handleTabClick={handleChangeTab}/>
        <SidebarDetails className={{flexGrow: 2}}>
            {navItemDefs.find(({key}) => key === selectedDetailsKey)['details']}
        </SidebarDetails>
    </div>;
}

export default SideBar;
