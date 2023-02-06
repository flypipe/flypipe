import React, {useState} from 'react';


const NodeSearchResultItem = ({name, nodePath, description}) => {
    const [isChecked, setIsChecked] = useState(false);
    return <div>
        <input id={`nodeCheckbox_${name}`} className="form-check-input mx-2" type="checkbox" name="nodeBuilderNodes" defaultChecked={isChecked} value={name}/>
        <label className="form-check-label" htmlFor={`nodeCheckbox_${name}`}><span className="fw-bold">{name}</span></label>
        <p className="fst-italic">{nodePath}</p>
        <p>{description}</p>
    </div>
};

export default NodeSearchResultItem;