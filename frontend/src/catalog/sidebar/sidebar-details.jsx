import React from 'react';
import Item from './item';


const SidebarDetails = ({children}) => {
    return <div className="d-flex flex-column">
        {children}
    </div>
}

export default SidebarDetails;