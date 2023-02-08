import React from 'react';
import CountBox from './count-box';


const CountBar = ({countBoxDefs}) => {
    return <div className="d-flex flex-wrap">
        {countBoxDefs.map(({label, count}) => <CountBox key={`key-${label}`} label={label} count={count}/>)}
    </div>
};


export default CountBar;