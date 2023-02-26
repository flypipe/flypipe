import React from "react";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import ReactBootstrapTooltip from "react-bootstrap/Tooltip";

const Tooltip = ({ text, children, placement = "right" }) => {
    return (
        <OverlayTrigger
            placement={placement}
            overlay={<ReactBootstrapTooltip>{text}</ReactBootstrapTooltip>}
        >
            {children}
        </OverlayTrigger>
    );
};

export default Tooltip;
