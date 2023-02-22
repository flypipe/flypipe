import React from 'react';
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import ReactBootstrapTooltip from "react-bootstrap/Tooltip";


const Tooltip = ({text, children}) => {
    return <OverlayTrigger
        key="right"
        placement="right"
        overlay={
            <ReactBootstrapTooltip id="tooltip-right">{text}</ReactBootstrapTooltip>
        }
    >
        {children}
    </OverlayTrigger>;
}


export default Tooltip;