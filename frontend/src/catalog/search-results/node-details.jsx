import React from 'react';


const NodeDetails = ({node}) => {
    const {name, sourceCode} = node;
    return <div className="p-4 h-100">
        <h3>{name}</h3>
        <pre className="border p-4 shadow-sm rounded h-75">
            {sourceCode}
        </pre>
    </div>
};

export default NodeDetails;
