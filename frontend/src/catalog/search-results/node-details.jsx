import React from 'react';
import SyntaxHighlighter from 'react-syntax-highlighter';


const NodeDetails = ({node}) => {
    const {name, sourceCode} = node;
    return <div className="p-4 h-100">
        <h3>{name}</h3>
        <SyntaxHighlighter
            language="python"
            className="border p-4 shadow-sm rounded h-75"
        >
            {sourceCode}
        </SyntaxHighlighter>
    </div>
};

export default NodeDetails;
