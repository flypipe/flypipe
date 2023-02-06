import React, {useState, useCallback} from 'react';
import NavTab from './nav-tab';


const NavTabs = ({tabDefs, handleTabClick}) => {
    const [selectedTab, setSelectedTab] = useState(tabDefs.length > 0 ? tabDefs[0].key : null);
    return <div className="tabs m-2">
        <nav className="navbar navbar-expand-lg bg-body-tertiary">
            <ul className="navbar-nav">
                {tabDefs.map(
                    ({key, title, details}) => <NavTab key={key} title={title} isSelected={selectedTab === key} onClick={(e) => {
                        setSelectedTab(key);
                        handleTabClick(key);
                    }}/>
                )}
            </ul>
        </nav>
    </div>
};

export default NavTabs;