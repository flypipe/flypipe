import React, { useState } from "react";

export const GraphContext = React.createContext();

export const GraphContextProvider = ({ children }) => {
    const [currentGraphObject, setCurrentGraphObject] = useState({
        object: null,
        type: null,
    });

    return (
        <GraphContext.Provider
            value={{ currentGraphObject, setCurrentGraphObject }}
        >
            {children}
        </GraphContext.Provider>
    );
};
