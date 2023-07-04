use std::env;
use num_cpus;
use rand::Rng;
use rayon::prelude::*;
use std::time::Instant;
use rand::seq::SliceRandom;
use rayon::ThreadPoolBuilder;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use calamine::{Reader, Xlsx, open_workbook};

struct ArgumentKind {
    input: Option<String>,
    output: Option<String>,
    config: Option<String>,
}

#[derive(Clone, Copy)]
struct ConfigKind {
    colony_size: usize,
    candidate_amount: usize,
    max_unimproved: usize,
    max_iterations: usize,
    improvement_threshold: f64,
    concurrent_count: usize,
    generation_method: GenerationMethod,
}

#[derive(Clone, Copy, PartialEq)]
enum GenerationMethod {
    None,
    Swap,
    Insert,
    Reverse,
    PartialShuffle,
}

fn get_arguments() -> ArgumentKind {
    let mut arguments = ArgumentKind {
        input: None,
        output: None,
        config: None,
    };
    let command_line: Vec<String> = env::args().collect();
    for argument in &command_line[1..] {
        let parts: Vec<&str> = argument.splitn(2, '=').collect();
        if parts.len() != 2 {
            panic!("Invalid argument.");
        }
        let key = parts[0];
        let value = parts[1].trim_matches('"').trim_matches('\'');
        match key {
            "--input" => arguments.input = Some(value.to_string()),
            "--output" => arguments.output = Some(value.to_string()),
            "--config" => arguments.config = Some(value.to_string()),
            _ => panic!("Unknown argument."),
        }
    }
    arguments
}

fn read_xlsx(input_path: String) -> Vec<Vec<f64>> {
    let mut xlsx_data: Vec<Vec<f64>> = Vec::new();
    let mut xlsx_file: Xlsx<_> = open_workbook(input_path).expect("Cannot open file.");
    let sheet_name = xlsx_file.sheet_names().get(0).expect("No data sheet found.").clone();
    if let Some(Ok(sheet)) = xlsx_file.worksheet_range(sheet_name.as_str()) {
        for row in sheet.rows() {
            let mut row_data: Vec<f64> = Vec::new();
            for col in row.iter() {
                let col_data = match col {
                    calamine::DataType::Int(i) => *i as f64,
                    calamine::DataType::Float(f) => *f,
                    _ => panic!("Invalid value in data sheet."),
                };
                row_data.push(col_data);
            }
            xlsx_data.push(row_data);
        }
    }
    xlsx_data
}

fn read_config(config_path: String) -> ConfigKind {
    let mut config = ConfigKind {
        colony_size: 0,
        candidate_amount: 0,
        max_unimproved: 0,
        max_iterations: 0,
        improvement_threshold: 0.0,
        concurrent_count: 0,
        generation_method: GenerationMethod::None,
    };
    let config_file = File::open(config_path).expect("Fail read config file.");
    let reader = BufReader::new(config_file);
    for line in reader.lines() {
        if let Ok(line) = line {
            let parts: Vec<&str> = line.split('=').map(|part| part.trim()).collect();
            if parts.len() == 2 {
                let key = parts[0];
                let value = parts[1];
                match key {
                    "colony_size" => config.colony_size = value.parse::<usize>().expect("Invalid configuration."),
                    "candidate_amount" => config.candidate_amount = match value {
                        "Default" => 0,
                        _ => value.parse::<usize>().expect("Invalid configuration."),
                    },
                    "max_unimproved" => config.max_unimproved = value.parse::<usize>().expect("Invalid configuration."),
                    "max_iterations" => config.max_iterations = value.parse::<usize>().expect("Invalid configuration."),
                    "improvement_threshold" => config.improvement_threshold = value.parse::<f64>().expect("Invalid configuration."),
                    "concurrent_count" => config.concurrent_count = match value {
                        "Default" => num_cpus::get(),
                        _ => value.parse::<usize>().expect("Invalid configuration."),
                    },
                    "generation_method" => config.generation_method = match value {
                        "Swap" => GenerationMethod::Swap,
                        "Insert" => GenerationMethod::Insert,
                        "Reverse" => GenerationMethod::Reverse,
                        "PartialShuffle" => GenerationMethod::PartialShuffle,
                        _ => panic!("Unknown configuration."),
                    },
                    _ => panic!("Unknown configuration."),
                }
            } else {
                panic!("Invalid configuration.")
            }
        } else {
            panic!("Fail read config file.");
        }
    }
    if config.candidate_amount == 0 {
        config.candidate_amount = config.colony_size / 2;
    }
    if config.concurrent_count == 0 {
        config.concurrent_count = num_cpus::get();
    }
    config
}

fn euclidean_distance(city1: &Vec<f64>, city2: &Vec<f64>) -> f64 {
    if city1.len() != city2.len() {
        panic!("Invalid data sheet.");
    }
    let mut distance = 0.0;
    for dimension in 0..city1.len() {
        distance += (city1[dimension] - city2[dimension]).powf(2.0);
    }
    distance.sqrt()
}

fn calc_cities_distance(cities: &Vec<Vec<f64>>) -> Vec<Vec<f64>> {
    let mut adjacency_matrix: Vec<Vec<f64>> = vec![vec![0.0; cities.len()]; cities.len()];
    for i in 0..cities.len() {
        for j in (i+1)..cities.len() {
            let distance = euclidean_distance(&cities[i], &cities[j]);
            adjacency_matrix[i][j] = distance;
            adjacency_matrix[j][i] = distance;
        }
    }
    adjacency_matrix
}

fn validate_config(config: &ConfigKind) {
    if config.colony_size < 1 || (config.colony_size % 2) != 0 {
        panic!("Invalid colony size.");
    } else if config.max_unimproved < 1 {
        panic!("Invalid unimproved times.");
    } else if config.max_iterations < 1 {
        panic!("Invalid iterations");
    } else if config.improvement_threshold < 0.0 || config.improvement_threshold > 100.0 {
        panic!("Invalid improvement threshold.");
    } else if config.candidate_amount < 1 {
        panic!("Invalid candidate amount.");
    } else if config.concurrent_count < 1 {
        panic!("Invalid concurrent count.");
    } else if config.generation_method == GenerationMethod::None {
        panic!("Invalid generation method.");
    }
}

fn initialize_solution(city_amount: usize) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut solution: Vec<usize> = (0..city_amount).collect();
    solution.shuffle(&mut rng);
    solution
}

fn calc_path_length(solution: &Vec<usize>, distance: &Vec<Vec<f64>>) -> f64 {
    let mut length = 0.0;
    for i in 0..(solution.len()-1) {
        length += distance[solution[i]][solution[i+1]];
    }
    length += distance[solution[solution.len()-1]][solution[0]];
    length
}

fn initialize_phase(distance: &Vec<Vec<f64>>, config: &ConfigKind) -> (Vec<Vec<usize>>, Vec<f64>) {
    let colony_size = config.colony_size;
    let concurrent_count = config.concurrent_count;
    let city_amount = distance.len();
    let thread_pool = ThreadPoolBuilder::new().num_threads(concurrent_count).build().expect("Fail build thread pool.");
    let solutions: Vec<Vec<usize>> = thread_pool.install(
        || {
            let solutions = (0..(colony_size / 2))
                .into_par_iter()
                .map(|_| initialize_solution(city_amount))
                .collect();
            solutions
        }
    );
    let solutions_length = thread_pool.install(
        || {
            let solutions_length: Vec<f64> = solutions
                .clone()
                .into_par_iter()
                .map(|solution| calc_path_length(&solution, &distance))
                .collect();
            solutions_length
        }
    );
    (solutions, solutions_length)
}

fn swap(solution: &Vec<usize>) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut neighbor = solution.clone();
    let (city1, city2) = loop {
        let (i, j) = (rng.gen_range(0..solution.len()), rng.gen_range(0..solution.len()));
        if i == j {
            continue;
        } else {
            break (i, j);
        }
    };
    neighbor.swap(city1, city2);
    neighbor
}

fn insert(solution: &Vec<usize>) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut neighbor = solution.clone();
    let (mut city1, mut city2) = loop {
        let (i, j) = (rng.gen_range(0..solution.len()), rng.gen_range(0..solution.len()));
        if i == j {
            continue;
        } else {
            break (i, j);
        }
    };
    if city1 > city2 {
        std::mem::swap(&mut city1, &mut city2);
    }
    let moved_city = neighbor.remove(city2);
    neighbor.insert(city1 + 1, moved_city);
    neighbor
}

fn reverse (solution: &Vec<usize>) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut neighbor = solution.clone();
    let (mut city1, mut city2) = loop {
        let (i, j) = (rng.gen_range(0..solution.len()), rng.gen_range(0..solution.len()));
        if i == j {
            continue;
        } else {
            break (i, j);
        }
    };
    if city1 > city2 {
        std::mem::swap(&mut city1, &mut city2);
    }
    neighbor[city1..=city2].reverse();
    neighbor
}

fn partial_shuffle (solution: &Vec<usize>) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let mut neighbor = solution.clone();
    let (mut city1, mut city2) = loop {
        let (i, j) = (rng.gen_range(0..solution.len()), rng.gen_range(0..solution.len()));
        if i == j {
            continue;
        } else {
            break (i, j);
        }
    };
    if city1 > city2 {
        std::mem::swap(&mut city1, &mut city2);
    }
    let partial = &mut neighbor[city1..=city2];
    partial.shuffle(&mut rng);
    neighbor
}

fn employed_bee(solution: &Vec<usize>, distance: &Vec<Vec<f64>>, config: &ConfigKind) -> Vec<usize> {
    let candidate_amount = config.candidate_amount;
    let generation_method = config.generation_method;
    let mut candidate_solution: Vec<Vec<usize>> = Vec::new();
    for _ in 0..candidate_amount {
        match generation_method {
            GenerationMethod::None => panic!("Unknown error."),
            GenerationMethod::Swap => {
                candidate_solution.push(swap(solution));
            },
            GenerationMethod::Insert => {
                candidate_solution.push(insert(solution));
            },
            GenerationMethod::Reverse => {
                candidate_solution.push(reverse(solution));
            },
            GenerationMethod::PartialShuffle => {
                candidate_solution.push(partial_shuffle(solution));
            },
        }
    }
    onlooker_bee(&candidate_solution, &distance)
}

fn onlooker_bee(candidate_solution: &Vec<Vec<usize>>, distance: &Vec<Vec<f64>>) -> Vec<usize> {
    let mut rng = rand::thread_rng();
    let candidate_amount = candidate_solution.len();
    let mut selected: Vec<usize> = Vec::new();
    while selected.len() < candidate_amount {
        let selected_number1 = rng.gen_range(0..candidate_amount);
        let selected_number2 = rng.gen_range(0..candidate_amount);
        if selected_number1 == selected_number2 {
            continue;
        }
        let selected_candidate1 = &candidate_solution[selected_number1];
        let selected_candidate2 = &candidate_solution[selected_number2];
        if calc_path_length(selected_candidate1, &distance) > calc_path_length(selected_candidate2, &distance) {
            selected.push(selected_number1);
        } else {
            selected.push(selected_number2);
        }
    }
    let mut count: Vec<usize> = vec![0; candidate_amount];
    for &number in &selected {
        count[number] += 1;
    }
    let max_count = *count.iter().max().unwrap();
    let max_number = count.iter().position(|&count| count == max_count).unwrap();
    candidate_solution[max_number].clone()
}

fn exploration_phase(solutions: &Vec<Vec<usize>>, distance: &Vec<Vec<f64>>, config: &ConfigKind) -> (Vec<Vec<usize>>, Vec<f64>) {
    let concurrent_count = config.concurrent_count;
    let thread_pool = ThreadPoolBuilder::new().num_threads(concurrent_count).build().expect("Fail build thread pool.");
    let new_solutions = thread_pool.install(
        || {
            let new_solutions: Vec<Vec<usize>> = solutions
                .clone()
                .into_par_iter()
                .map(|solution| employed_bee(&solution, distance, config))
                .collect();
            new_solutions
        }
    );
    let new_solutions_length = thread_pool.install(
        || {
            let new_solutions_length: Vec<f64> = new_solutions
                .clone()
                .into_par_iter()
                .map(|solution| calc_path_length(&solution, distance))
                .collect();
            new_solutions_length
        }
    );
    (new_solutions, new_solutions_length)
}

fn artificial_bee_colony(distance: &Vec<Vec<f64>>, config: &ConfigKind) -> (Vec<usize>, f64) {
    let city_amount = distance.len();
    let colony_size = config.colony_size;
    let max_iterations= config.max_iterations;
    let max_unimproved = config.max_unimproved;
    let improvement_threshold = config.improvement_threshold;
    let (mut solutions, mut solutions_length) = initialize_phase(&distance, &config);
    let mut best_solution: Vec<usize> = solutions[0].clone();
    let mut best_solution_length = solutions_length[0];
    let mut unimproved_times: Vec<usize> = vec![0; colony_size / 2];
    for _ in 0..max_iterations {
        let (mut new_solutions, mut new_solutions_length) = exploration_phase(&solutions, &distance, &config);
        for index in 0..(colony_size / 2) {
            if new_solutions_length[index] < solutions_length[index] {
                solutions[index] = new_solutions[index].clone();
                solutions_length[index] = new_solutions_length[index];
                unimproved_times[index] = 0;
            } else {
                unimproved_times[index] += 1;
            }
        }
        for index in 0..(colony_size / 2) {
            if unimproved_times[index] > max_unimproved {
                solutions[index] = initialize_solution(city_amount);
                solutions_length[index] = calc_path_length(&solutions[index], &distance);
                unimproved_times[index] = 0;
            }
        }
        let best_index = solutions_length.iter().enumerate().min_by(|&(_, length1), &(_, length2)| length1.partial_cmp(length2).unwrap()).unwrap().0;
        if solutions_length[best_index] < best_solution_length {
            let improvement = (best_solution_length - solutions_length[best_index]) / best_solution_length;
            best_solution = solutions[best_index].clone();
            best_solution_length = solutions_length[best_index];
            if improvement < improvement_threshold {
                break;
            }
        }
    }
    (best_solution, best_solution_length)
}

fn write_result(output_path: String, output_message: String) {
    let mut output_file = match OpenOptions::new().read(true).write(true).create(true).truncate(true).open(output_path) {
        Ok(output_file) => output_file,
        Err(_) => panic!("Failed to open or create file."),
    };
    if let Err(e) = output_file.write_all(output_message.as_bytes()) {
        panic!("Failed to write to file.\nReason: {}", e);
    }
}

fn main() {
    let start_time = Instant::now();
    let arguments = get_arguments();
    let input_path = arguments.input.expect("Missing argument.");
    let output_path = arguments.output.expect("Missing argument.");
    let config_path = arguments.config.expect("Missing argument.");
    let cities = read_xlsx(input_path);
    let distance = calc_cities_distance(&cities);
    let config = read_config(config_path);
    validate_config(&config);
    let (best_solution, best_solution_length) = artificial_bee_colony(&distance, &config);
    let mut output_message = String::new();
    let solution_format: Vec<String> = best_solution.iter().map(|city| city.to_string()).collect();
    output_message.push_str(&format!("Best solution:{}\n", solution_format.join(" ")));
    output_message.push_str(&format!("Best solution length:{}\n", best_solution_length));
    output_message.push_str(&format!("Cost time:{:?}\n", start_time.elapsed()));
    write_result(output_path, output_message);
}
