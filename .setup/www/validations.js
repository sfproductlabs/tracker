import defaultTo from '../utils/defaultTo';

const validateRequired = function(data) {
    data = defaultTo('')(data);
    const re = /\S/i;
    if(!re.test(data))
        return 'Is Required;';
    else 
        return null;
}

const validateUuid = function(data) {
    const re = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if(!re.test(data))
        return 'Invalid Id;';
    else 
        return null;
}


const validateEmail = function(data) {
    const re = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;
    if(!re.test(data))
        return 'Not an email;';
    else
        return null;
}

const validate = function(data, validations) {
    let errors = {};
    const re = /\S/i;
    Object.keys(validations).map(function(key,index){
        let toCheck = defaultTo('')(validations[key])
        if(re.test(toCheck)) {
            //Split validations and test each one, then add to error object
            let checkAll = validations[key].split(",");
            checkAll.map(function(v,i) {
                if (v.length > 0) {
                    let m = module.exports['validate' +  v[0].toUpperCase() + v.slice(1).toLowerCase()];
                    errors[key] = (m(data[key]) || '') + (errors[key] || '');
                }
            });
        }
        
    });
    return errors; 
}

const hasErrors = function(errors) {
    const re = /\S/i;
    let toReturn = false;
    Object.keys(errors).map(function(key,index){
        let toCheck = defaultTo('')(errors[key]);
        if(re.test(toCheck))
            toReturn = true;       
    });
    return toReturn;
}

module.exports = {
    hasErrors,
    validateEmail,
    validateRequired,
    validateUuid,
    validate
}