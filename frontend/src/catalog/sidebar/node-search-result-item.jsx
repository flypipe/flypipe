import React, {useCallback} from 'react';


const NodeSearchResultItem = ({name, importCmd, description, checked, handleCheck}) => {
    // const [isChecked, setIsChecked] = useState(false);
    const onCheck = useCallback((e) => {
        handleCheck(e);
    }, [handleCheck]);
    return <div>
        <input id={`nodeCheckbox_${name}`} className="form-check-input mx-2" type="checkbox" name="nodeBuilderNodes" defaultChecked={checked} onClick={onCheck} value={name}/>
        <label className="form-check-label" htmlFor={`nodeCheckbox_${name}`}><span className="fw-bold">{name}</span></label>
        <br/>
        <code>{importCmd}</code>
        <p>{description}</p>
    </div>
};

export default NodeSearchResultItem;