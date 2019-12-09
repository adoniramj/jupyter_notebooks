// Multiplying an array of numbers by 10

var original_array = [1,2,3,4,5]

var new_array = original_array.map(element => element * 10)

console.log(new_array)

// Map over an array of objects
var array_of_objects = [
                      { key:1, value:10},
                      { key:2, value:20},
                      { key:3, value:30}
                      ]

var new_array_of_objects = array_of_objects.map(obj => {
    var temp_obj = {}
    temp_obj[obj.key] = obj.value
    return temp_obj
})
console.log(new_array_of_objects)
console.log(new_array_of_objects[0]['1'])




