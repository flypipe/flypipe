import React, { useState, useCallback } from "react";
import { getNewNodeDef } from "../util";

export const NodeDetailsContext = React.createContext();

export const NodeDetailsContextProvider = ({ children }) => {
    const [nodeDetailsState, setNodeDetailsState] = useState({
        nodeData: getNewNodeDef({}),
        visible: false,
    });
    const openNodeDetails = useCallback(
        (nodeData) => {
            setNodeDetailsState({
                nodeData,
                visible: true,
            });
        },
        [setNodeDetailsState]
    );

    return (
        <NodeDetailsContext.Provider
            value={{ openNodeDetails, nodeDetailsState, setNodeDetailsState }}
        >
            {children}
        </NodeDetailsContext.Provider>
    );
};
