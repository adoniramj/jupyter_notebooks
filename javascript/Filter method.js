// Return a new array with no negative numbers

let original_array = [1,-5,10,4,-3,6,-67]
let new_array = original_array.filter(element => element > 0)
console.log(new_array)
console.log(original_array)

// Filtering negative numbers using a function

function neg_num(num){
    return num < 0
}

let neg_array = original_array.filter(neg_num)

console.log(neg_array)

// Return California
let states = ['Florida', 'California', 'New York']

let new_states = states.filter(element => element === 'California')
console.log(new_states)


