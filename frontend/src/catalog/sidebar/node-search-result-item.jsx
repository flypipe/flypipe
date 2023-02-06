import React, {useState} from 'react';


const NodeSearchResultItem = ({name, nodePath, description}) => {
    const [isChecked, setIsChecked] = useState(false);
    return <div>
        <input type="checkbox" defaultChecked={isChecked}></input>
        <p>{name}</p>
        <p className="font-italic">{nodePath}</p>
        <p>{description}</p>
    </div>
};

export default NodeSearchResultItem;